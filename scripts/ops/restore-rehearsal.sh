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
#   scripts/ops/restore-rehearsal.sh <백업이름>        예) incr_20260920
#   scripts/ops/restore-rehearsal.sh <백업이름> --keep  검증 뒤 복원본을 남긴다(조사용)
set -euo pipefail
cd "$(dirname "$0")/../.."
[ -f .env ] || { echo ".env 없음"; exit 1; }
set -a; . ./.env; set +a

BACKUP=${1:?백업 이름이 필요하다 (예: incr_20260920)}
KEEP=${2:-}
DB=cdc_restore_test
# 검증 대상: 가장 큰 원장 표 + 파생 표 + 차원 표. 종류가 다르면 깨지는 방식도 다르다.
TABLES=(crypto_trades binance_orderbook_raw dim_market_state_scd)
say(){ echo "[$(date -u +%T)] $*"; }
CH(){ docker exec cdc-clickhouse clickhouse-client -q "$1"; }
ADMIN(){ docker exec cdc-clickhouse clickhouse-client --user default --password "$CLICKHOUSE_DEFAULT_PASSWORD" -q "$1"; }

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
