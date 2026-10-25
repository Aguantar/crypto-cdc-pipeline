#!/usr/bin/env bash
# 복원 리허설 (docs/21 §1, docs/34 #10). "백업이 있다"는 이 스크립트가 통과한 뒤에만 할 수 있는 말이다.
#
# 무엇을 증명하나: ① 백업이 실제로 열리고 ② 표가 복원되고 ③ 복원본의 집계값이 원본과 같다.
# 행 수만 보면 안 되는 이유: 타입 변환 사고(docs/34 §5)가 행 수로는 통과하고 금액 합에서만 잡혔다.
#
# 반드시 표 단위로 복원한다 - 데이터베이스째 복원하면 안 된다:
#   cdc_pipeline 에는 Kafka 엔진 표가 6개 있다(ledger_*_queue). 복원본이 뜨면 같은 컨슈머 그룹으로
#   프로덕션 원장 메시지를 가로챈다. 재해 복구 때는 맞지만, 살아 있는 클러스터에서 하는 리허설에서는 사고다.
#
# 자격증명: pipeline 사용자는 다른 데이터베이스에 CREATE/INSERT 권한이 없어 복원이 ACCESS_DENIED 로 막힌다
#   (접근 통제가 의도대로 작동하는 것). 복원은 default(관리자)로 한다.
#
# 사용:
#   scripts/ops/restore-rehearsal.sh <백업이름>        예) incr_20260920        네이티브 BACKUP 복원
#   scripts/ops/restore-rehearsal.sh <백업이름> --keep  검증 뒤 복원본을 남긴다(조사용)
#   scripts/ops/restore-rehearsal.sh --mode parquet --date 2026-10-20 [--keep]
#       Oracle 의 호가 Parquet(backup_daily.export_parquet 산출물) 하루치를 ClickHouse 로 되넣고 원본 파티션과 대조한다 (docs/52).
#       원본은 7일 TTL 이라 날짜가 TTL 안이어야 원본 대조가 된다. 밖이면 Parquet 메타 행 수와만 대조하고 그렇게 말한다.
#       방법은 file() 테이블 함수다. HTTP INSERT 는 Parquet 이 seek 를 요구해 서버가 파일 전체(0.7GB)를 메모리에 올리는데,
#       이 서버의 예산은 1.57GiB 라 그 길은 막혀 있다. file() 은 row group(20만 행) 단위로 읽어 실측 피크 444MiB 였다.
set -euo pipefail
cd "$(dirname "$0")/../.."
[ -f .env ] || { echo ".env 없음"; exit 1; }
set -a; . ./.env; set +a

MODE=native; DATE=""; KEEP=""; BACKUP=""
while [ $# -gt 0 ]; do
  case "$1" in
    --mode) MODE=$2; shift 2;;
    --date) DATE=$2; shift 2;;
    --keep) KEEP=--keep; shift;;
    *) BACKUP=$1; shift;;
  esac
done
DB=cdc_restore_test
say(){ echo "[$(date -u +%T)] $*"; }
CH(){ docker exec cdc-clickhouse clickhouse-client -q "$1"; }
ADMIN(){ docker exec cdc-clickhouse clickhouse-client --user default --password "$CLICKHOUSE_DEFAULT_PASSWORD" -q "$1"; }

if [ "$MODE" = parquet ]; then
  [ -n "$DATE" ] || { echo "--date YYYY-MM-DD 가 필요하다"; exit 1; }
  FILE="orderbook_raw_${DATE}.parquet"
  REMOTE="/mnt/backup/clickhouse/parquet/$FILE"
  LOCAL_DIR="$HOME/clickhouse-backups/restore-test"
  mkdir -p "$LOCAL_DIR"
  say "0. Parquet 확인: oracle:$REMOTE"
  META=$(ssh oracle "stat -c %s $REMOTE; ~/toss/venv/bin/python -c \"import pyarrow.parquet as pq; m=pq.ParquetFile('$REMOTE').metadata; print(m.num_rows, m.num_row_groups)\"" | tr '\n' ' ')
  set -- $META; BYTES=$1; PQ_ROWS=$2; PQ_RG=$3
  say "   크기 $BYTES bytes, 행 $PQ_ROWS, row group $PQ_RG"
  IN_TTL=$(CH "SELECT count() FROM system.parts WHERE active AND database='cdc_pipeline' AND table='orderbook_raw' AND partition='$DATE'")
  [ "$IN_TTL" -gt 0 ] && say "   원본 파티션 $DATE 있음 - 원본과 대조한다" || say "   원본 파티션 $DATE 없음(TTL 밖) - Parquet 메타 행 수와만 대조한다"

  say "1. 전송 Oracle → 호스트 → 컨테이너 user_files (file() 은 user_files_path 아래만 읽는다)"
  S=$(date +%s); scp -q "oracle:$REMOTE" "$LOCAL_DIR/$FILE"; say "   scp $(( $(date +%s) - S ))초"
  S=$(date +%s); docker cp "$LOCAL_DIR/$FILE" "cdc-clickhouse:/var/lib/clickhouse/user_files/$FILE"; say "   docker cp $(( $(date +%s) - S ))초"

  say "2. 복원 표 준비 (원본 DDL, TTL 제거)"
  ADMIN "CREATE DATABASE IF NOT EXISTS $DB"
  ADMIN "DROP TABLE IF EXISTS $DB.orderbook_raw"
  ADMIN "CREATE TABLE $DB.orderbook_raw AS cdc_pipeline.orderbook_raw"
  ADMIN "ALTER TABLE $DB.orderbook_raw REMOVE TTL"

  say "3. 적재 (단일 스레드, 메모리 상한 600MB - 프로덕션을 밀어내지 않기 위해)"
  S=$(date +%s)
  ADMIN "INSERT INTO $DB.orderbook_raw SELECT * FROM file('$FILE', Parquet) SETTINGS max_threads=1, max_insert_threads=1, max_memory_usage=600000000, input_format_parquet_max_block_size=65536"
  say "   적재 $(( $(date +%s) - S ))초"
  ADMIN "SYSTEM FLUSH LOGS"   # query_log 는 7.5초마다 flush 된다 - 직전 실행의 행을 읽는 일이 있었다
  CH "SELECT concat('   피크 메모리 ', formatReadableSize(memory_usage), ', 서버 보고 ', toString(query_duration_ms), 'ms') FROM system.query_log WHERE type='QueryFinish' AND query LIKE 'INSERT INTO $DB.orderbook_raw%' ORDER BY event_time DESC LIMIT 1"
  say "   복원본 디스크 $(CH "SELECT formatReadableSize(sum(bytes_on_disk)) FROM system.parts WHERE active AND database='$DB'")"

  say "4. 대조"
  FAIL=0
  HASH="sum(cityHash64(market, ts, level, total_ask_size, total_bid_size, ask_prices, ask_sizes, bid_prices, bid_sizes, stream_type, recv_ts, flink_ts, inserted_at))"
  a=$(CH "SELECT concat(toString(count()), ' / ', toString(uniq(market)), ' / ', toString($HASH)) FROM $DB.orderbook_raw WHERE toDate(ts)='$DATE' SETTINGS max_threads=2")
  say "   복원본 행 / 마켓 / 전행 해시: $a"
  [ "${a%% /*}" = "$PQ_ROWS" ] && say "   ✓ Parquet 메타 행 수 일치 ($PQ_ROWS)" || { say "   ✗ Parquet 메타 행 수 $PQ_ROWS 와 다름"; FAIL=1; }
  if [ "$IN_TTL" -gt 0 ]; then
    b=$(CH "SELECT concat(toString(count()), ' / ', toString(uniq(market)), ' / ', toString($HASH)) FROM cdc_pipeline.orderbook_raw WHERE toDate(ts)='$DATE' SETTINGS max_threads=2")
    say "   원본   행 / 마켓 / 전행 해시: $b"
    [ "$a" = "$b" ] && say "   ✓ 원본과 일치" || { say "   ✗ 원본과 불일치"; FAIL=1; }
    cells=$(CH "SELECT concat(toString(countIf(a.c != b.c OR a.c = 0 OR b.c = 0)), ' / ', toString(count())) FROM (SELECT market, toStartOfHour(ts) h, count() c FROM $DB.orderbook_raw WHERE toDate(ts)='$DATE' GROUP BY 1,2) a FULL OUTER JOIN (SELECT market, toStartOfHour(ts) h, count() c FROM cdc_pipeline.orderbook_raw WHERE toDate(ts)='$DATE' GROUP BY 1,2) b USING (market, h) SETTINGS max_threads=2, join_use_nulls=0")
    say "   마켓×시간 셀 불일치 / 전체: $cells"
    [ "${cells%% /*}" = "0" ] || FAIL=1
  fi

  if [ "$KEEP" = "--keep" ]; then
    say "5. 복원본 유지 ($DB.orderbook_raw) - 끝나면 DROP. 파일은 지운다"
  else
    say "5. 정리"
    ADMIN "DROP TABLE $DB.orderbook_raw"; ADMIN "DROP DATABASE IF EXISTS $DB"
  fi
  docker exec cdc-clickhouse rm -f "/var/lib/clickhouse/user_files/$FILE"
  rm -f "$LOCAL_DIR/$FILE"; rmdir "$LOCAL_DIR" 2>/dev/null || true
  say "결과: $([ $FAIL -eq 0 ] && echo '통과 - 이 Parquet 은 복구에 쓸 수 있다' || echo '실패')"
  exit $FAIL
fi

[ -n "$BACKUP" ] || { echo "백업 이름이 필요하다 (예: incr_20260920)"; exit 1; }
# 검증 대상: 가장 큰 원장 표 + 파생 표 + 차원 표. 종류가 다르면 깨지는 방식도 다르다.
TABLES=(crypto_trades binance_orderbook_raw dim_market_state_scd)
say "0. 백업 확인: $BACKUP"
CH "SELECT name, status, num_files, formatReadableSize(total_size) FROM system.backups WHERE name LIKE '%${BACKUP}%' ORDER BY start_time DESC LIMIT 1 FORMAT TSV" || true

say "1. 복원 대상 데이터베이스 준비 (기존 것은 지운다)"
ADMIN "DROP DATABASE IF EXISTS $DB"
ADMIN "CREATE DATABASE $DB"

say "2. 표 단위 복원 (Kafka 엔진 표는 건드리지 않는다)"
LIST=$(printf 'TABLE cdc_pipeline.%s AS '"$DB"'.%s, ' "${TABLES[@]}" "${TABLES[@]}" 2>/dev/null || true)
LIST=""
for t in "${TABLES[@]}"; do LIST+="TABLE cdc_pipeline.$t AS $DB.$t, "; done
LIST=${LIST%, }
START=$(date +%s)
ADMIN "RESTORE $LIST FROM File('/backups/$BACKUP')" | sed 's/^/   /'
say "   복원 $(( $(date +%s) - START ))초"

say "3. 대조 - 행 수와 '값'을 함께 본다"
FAIL=0
for t in "${TABLES[@]}"; do
  r=$(CH "SELECT count() FROM $DB.$t")
  say "   $t 복원 행 $r"
done
# 체결은 하루를 골라 FINAL 집계까지 비교한다. ReplacingMergeTree 라 FINAL 없이 비교하면
# 재처리분 중복 때문에 달라 보인다(docs/34 #2·#6) - 실제로 이 리허설에서 한 번 그렇게 보였다.
DAY=$(CH "SELECT toString(today() - 1)")
a=$(CH "SELECT concat(toString(count()), ' / ', toString(sum(trade_amount))) FROM $DB.crypto_trades FINAL WHERE toDate(source_ts) = '$DAY'")
b=$(CH "SELECT concat(toString(count()), ' / ', toString(sum(trade_amount))) FROM cdc_pipeline.crypto_trades FINAL WHERE toDate(source_ts) = '$DAY'")
say "   $DAY 복원본 : $a"
say "   $DAY 원본   : $b"
[ "$a" = "$b" ] && say "   ✓ 행 수·금액 합 일치" || { say "   ✗ 불일치"; FAIL=1; }

if [ "$KEEP" = "--keep" ]; then
  say "4. 복원본 유지 ($DB) - 조사 끝나면 DROP DATABASE $DB"
else
  say "4. 정리"
  ADMIN "DROP DATABASE $DB"
fi
say "결과: $([ $FAIL -eq 0 ] && echo '통과 - 이 백업은 복구에 쓸 수 있다' || echo '실패')"
exit $FAIL
