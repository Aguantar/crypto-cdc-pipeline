#!/usr/bin/env bash
# A-1 런북: crypto_trades → 체결 시각 일 파티션 테이블로 무정지 교체 (docs/28 A-2·A-3·A-3-1). 단계별 실행: prepare | copy | swap | finalize | rollback
set -euo pipefail
cd "$(dirname "$0")/../.."
P=$(grep '^MYSQL_ROOT_PASSWORD=' .env | cut -d= -f2-)
MY(){ docker exec -i cdc-mysql mysql -uroot -p"$P" --default-character-set=utf8mb4 -e "$1" 2>&1 | grep -v "Warning: Using" || true; }
MYN(){ docker exec cdc-mysql mysql -uroot -p"$P" -N -e "$1" 2>/dev/null; }
CH(){ docker exec cdc-clickhouse clickhouse-client -q "$1"; }
say(){ echo "[$(date -u +%FT%TZ)] $*"; }
LOG=/home/calme/kafka-reassign/a1-swap-$(date -u +%Y%m%d).log; mkdir -p /home/calme/kafka-reassign
PHASE=${1:-status}
parts(){ python3 -c "
import datetime as dt
out=[]
for i in range(-8, 3):   # 오늘 −8일 ~ +2일
    d=dt.date.today()+dt.timedelta(days=i); n=int(dt.datetime(d.year,d.month,d.day,tzinfo=dt.timezone.utc).timestamp()*1000)//86400000
    out.append(f\"PARTITION p{d.strftime('%Y%m%d')} VALUES LESS THAN ({n+1})\")
print(', '.join(out))"; }

case "$PHASE" in
prepare)
  say "prepare: 새 테이블 생성 + 유지보수 프로시저" | tee -a $LOG
  MY "CREATE TABLE crypto_db.crypto_trades_p (
    trade_id BIGINT NOT NULL AUTO_INCREMENT,
    market VARCHAR(20) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    trade_price DECIMAL(20,8) NOT NULL, trade_volume DECIMAL(20,8) NOT NULL, trade_amount DECIMAL(20,4) NOT NULL,
    ask_bid CHAR(3) CHARACTER SET ascii NOT NULL,
    upbit_timestamp BIGINT NOT NULL, sequential_id BIGINT NOT NULL,
    recv_ms BIGINT NULL COMMENT 'producer WS 수신 epoch ms',
    created_at TIMESTAMP(3) NULL DEFAULT CURRENT_TIMESTAMP(3),
    best_ask_price DECIMAL(20,8) NULL, best_ask_size DECIMAL(20,8) NULL, best_bid_price DECIMAL(20,8) NULL, best_bid_size DECIMAL(20,8) NULL,
    ingest_source ENUM('ws','gapfill','backfill') NOT NULL DEFAULT 'ws',
    stream_type ENUM('REALTIME','SNAPSHOT') NOT NULL DEFAULT 'REALTIME',
    PRIMARY KEY (trade_id, upbit_timestamp),
    UNIQUE KEY uk_market_seq (market, sequential_id, upbit_timestamp),
    KEY idx_market_ts (market, upbit_timestamp)
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='체결 원장 v2 (체결 시각 일 파티션, docs/28)'
  PARTITION BY RANGE (upbit_timestamp DIV 86400000) ($(parts), PARTITION p_max VALUES LESS THAN MAXVALUE)"
  docker exec -i cdc-mysql mysql -uroot -p"$P" crypto_db < scripts/ops/manage_trade_partitions.sql 2>&1 | grep -v "Warning: Using" || true   # 프로시저는 DELIMITER 가 필요해 파일로 (mysql -e 는 본문 세미콜론에서 끊긴다)
  MYN "SELECT partition_name, partition_description FROM information_schema.partitions WHERE table_schema='crypto_db' AND table_name='crypto_trades_p' ORDER BY partition_ordinal_position" | tr '\t' ':' | tr '\n' ' ' | tee -a $LOG; echo
  ;;
copy)
  say "copy: 옛 테이블 → 새 테이블, 일 단위, sql_log_bin=0" | tee -a $LOG
  for d in $(MYN "SELECT DISTINCT FROM_UNIXTIME(upbit_timestamp DIV 1000, '%Y-%m-%d') FROM crypto_db.crypto_trades ORDER BY 1"); do
    s=$(date +%s); LO=$(date -u -d "$d" +%s)000; HI=$(date -u -d "$d +1 day" +%s)000
    MY "SET sql_log_bin=0; INSERT IGNORE INTO crypto_db.crypto_trades_p (trade_id, market, trade_price, trade_volume, trade_amount, ask_bid, upbit_timestamp, sequential_id, created_at, best_ask_price, best_ask_size, best_bid_price, best_bid_size) SELECT trade_id, market, trade_price, trade_volume, trade_amount, ask_bid, upbit_timestamp, sequential_id, created_at, best_ask_price, best_ask_size, best_bid_price, best_bid_size FROM crypto_db.crypto_trades WHERE upbit_timestamp >= $LO AND upbit_timestamp < $HI"
    say "  $d: src $(MYN "SELECT count(*) FROM crypto_db.crypto_trades WHERE upbit_timestamp >= $LO AND upbit_timestamp < $HI") dst $(MYN "SELECT count(*) FROM crypto_db.crypto_trades_p WHERE upbit_timestamp >= $LO AND upbit_timestamp < $HI") ($(( $(date +%s) - s ))s)" | tee -a $LOG
  done
  say "  p_max rows: $(MYN "SELECT count(*) FROM crypto_db.crypto_trades_p PARTITION (p_max)"), ch 60s: $(CH "SELECT count() FROM cdc_pipeline.crypto_trades WHERE flink_ts >= now() - INTERVAL 60 SECOND")" | tee -a $LOG
  ;;
verify)
  say "verify: 일별 src/dst 건수 대조 (스왑 전 필수)" | tee -a $LOG
  BAD=0
  for d in $(MYN "SELECT DISTINCT FROM_UNIXTIME(upbit_timestamp DIV 1000, '%Y-%m-%d') FROM crypto_db.crypto_trades ORDER BY 1"); do
    LO=$(date -u -d "$d" +%s)000; HI=$(date -u -d "$d +1 day" +%s)000
    a=$(MYN "SELECT count(*) FROM crypto_db.crypto_trades WHERE upbit_timestamp >= $LO AND upbit_timestamp < $HI"); b=$(MYN "SELECT count(*) FROM crypto_db.crypto_trades_p WHERE upbit_timestamp >= $LO AND upbit_timestamp < $HI")
    # 09-19 실측으로 정한 판정 규칙: 가장 오래된 날은 옛 테이블이 보존 정리(10분마다 DELETE)로 줄어드니 dst ≥ src 면 정상,
    # 오늘은 복사 뒤에도 옛 테이블에 계속 들어오니 src ≥ dst 면 정상(차이분은 swap 이 옮김). 그 사이 날들은 정확히 일치해야 한다.
    # 정정(02:16): 옛 테이블의 보존 정리는 created_at 기준이라 어느 날이든 줄 수 있다(09-12 가 줄고 09-10·11 백필 행은 아직). 복사는 INSERT IGNORE 라 dst 가 src 에 없던 행을 가질 수 없다
    # → 지난 날: dst ≥ src 면 정상(줄어든 만큼은 보존 정리), 오늘: src ≥ dst 면 정상(차이분은 swap 이 옮김)
    today=$(date -u +%Y-%m-%d)
    if [ "$d" = "$today" ]; then [ "$a" -ge "$b" ] && ok="ok(delta $((a-b)) → swap)" || { ok=MISMATCH; BAD=1; }
    else [ "$b" -ge "$a" ] && { [ "$a" = "$b" ] && ok=ok || ok="ok(retention shrank src by $((b-a)))"; } || { ok=MISMATCH; BAD=1; }; fi
    say "  $d src=$a dst=$b $ok" | tee -a $LOG
  done
  [ $BAD = 0 ] && say "  verify PASS" | tee -a $LOG || { say "  verify FAIL → copy 를 다시(INSERT IGNORE, 멱등)"; exit 1; }
  ;;
swap)
  say "swap: EVENT disable → AUTO_INCREMENT 여유 → RENAME → 차이분" | tee -a $LOG
  MY "ALTER EVENT crypto_db.cleanup_old_trades DISABLE"
  OLDMAX=$(MYN "SELECT max(trade_id) FROM crypto_db.crypto_trades"); MY "ALTER TABLE crypto_db.crypto_trades_p AUTO_INCREMENT = $((OLDMAX + 100000))"
  say "  old max trade_id=$OLDMAX, new AUTO_INCREMENT=$((OLDMAX + 100000))" | tee -a $LOG
  PRE_CH=$(CH "SELECT max(trade_id) FROM cdc_pipeline.crypto_trades")
  MY "RENAME TABLE crypto_db.crypto_trades TO crypto_db.crypto_trades_old, crypto_db.crypto_trades_p TO crypto_db.crypto_trades"; SWAP_T=$(date -u +%s)
  say "  renamed at $(date -u -d @$SWAP_T +%T)" | tee -a $LOG
  MY "SET sql_log_bin=0; INSERT IGNORE INTO crypto_db.crypto_trades (trade_id, market, trade_price, trade_volume, trade_amount, ask_bid, upbit_timestamp, sequential_id, created_at, best_ask_price, best_ask_size, best_bid_price, best_bid_size) SELECT trade_id, market, trade_price, trade_volume, trade_amount, ask_bid, upbit_timestamp, sequential_id, created_at, best_ask_price, best_ask_size, best_bid_price, best_bid_size FROM crypto_db.crypto_trades_old WHERE trade_id > (SELECT COALESCE(max(trade_id),0) FROM crypto_db.crypto_trades WHERE trade_id <= $OLDMAX)"
  say "  delta copied. old rows > $OLDMAX: $(MYN "SELECT count(*) FROM crypto_db.crypto_trades_old WHERE trade_id > $OLDMAX")" | tee -a $LOG
  sleep 30
  say "  verify: new inserts min id $(MYN "SELECT min(trade_id) FROM crypto_db.crypto_trades WHERE trade_id > $((OLDMAX + 100000 - 1))") (>= $((OLDMAX + 100000))), connect $(docker exec cdc-kafka-connect curl -s localhost:8083/connectors/mysql-cdc-connector/status | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['connector']['state'], [t['state'] for t in d['tasks']])"), ch first after pre-max $PRE_CH: $(CH "SELECT min(trade_id), count() FROM cdc_pipeline.crypto_trades WHERE trade_id > $PRE_CH FORMAT TSV" | tr '\t' '/'), ch 60s rows $(CH "SELECT count() FROM cdc_pipeline.crypto_trades WHERE flink_ts >= now() - INTERVAL 60 SECOND")" | tee -a $LOG
  docker logs cdc-kafka-connect --since 120s 2>&1 | grep -iE "Renaming|ERROR" | grep -v "errors\." | cut -c1-160 | tee -a $LOG
  ;;
finalize)
  say "finalize: 파티션 유지보수 EVENT 등록, 옛 EVENT 삭제, 상태" | tee -a $LOG
  MY "CREATE EVENT IF NOT EXISTS crypto_db.manage_trade_partitions_daily ON SCHEDULE EVERY 1 DAY STARTS (UTC_DATE() + INTERVAL 1 DAY + INTERVAL 5 MINUTE) DO CALL crypto_db.manage_trade_partitions()"
  MY "CALL crypto_db.manage_trade_partitions()"
  MY "DROP EVENT IF EXISTS crypto_db.cleanup_old_trades"
  MYN "SELECT partition_name, table_rows FROM information_schema.partitions WHERE table_schema='crypto_db' AND table_name='crypto_trades' ORDER BY partition_ordinal_position" | tr '\t' ':' | tr '\n' ' ' | tee -a $LOG; echo
  MYN "SELECT event_name, status, interval_value, interval_field FROM information_schema.events WHERE event_schema='crypto_db'" | tee -a $LOG
  ;;
rollback)
  say "rollback: 새→옛 차이분 복사, RENAME 되돌림, EVENT 복원" | tee -a $LOG
  MY "SET sql_log_bin=0; INSERT IGNORE INTO crypto_db.crypto_trades_old (trade_id, market, trade_price, trade_volume, trade_amount, ask_bid, upbit_timestamp, sequential_id, created_at, best_ask_price, best_ask_size, best_bid_price, best_bid_size) SELECT trade_id, market, trade_price, trade_volume, trade_amount, ask_bid, upbit_timestamp, sequential_id, created_at, best_ask_price, best_ask_size, best_bid_price, best_bid_size FROM crypto_db.crypto_trades WHERE trade_id > (SELECT max(trade_id) FROM crypto_db.crypto_trades_old)"
  MY "RENAME TABLE crypto_db.crypto_trades TO crypto_db.crypto_trades_p, crypto_db.crypto_trades_old TO crypto_db.crypto_trades"
  MY "ALTER EVENT crypto_db.cleanup_old_trades ENABLE"
  say "  rolled back" | tee -a $LOG
  ;;
status)
  MYN "SELECT table_name, table_rows, round(data_length/1048576) mb FROM information_schema.tables WHERE table_schema='crypto_db' AND table_name LIKE 'crypto_trades%'" | tr '\t' ' '
  ;;
esac
